use std::io;
use std::borrow::Borrow;
use std::hash::BuildHasher;
use std::rc::Rc;
use std::sync::Arc;
use compacts::bits;
use super::{Bytes, Seek, Store};
use super::cache::{self, Cache, RandomState};

#[derive(Debug)]
pub struct Index<S = Rc<Store>, H = RandomState>
where
    S: Borrow<Store>,
    H: BuildHasher,
{
    store: S,
    cache: cache::Single<Bytes, bits::Set, H>,
}

#[derive(Debug)]
pub struct SharedIndex<S = Arc<Store>, H = RandomState>
where
    S: Borrow<Store>,
    H: BuildHasher,
{
    store: S,
    cache: cache::Shared<Bytes, bits::Set, H>,
}

macro_rules! impls {
    ( $this:ident, $name:ident, $ptr:ident ) => {
        impl<S, H> $this<S, H>
        where
            S: Borrow<Store>,
            H: BuildHasher,
        {
            pub fn new(store: S, raw: cache::Raw<Bytes, $ptr<bits::Set>, H>) -> Self {
                let cache = {
                    let cap = raw.capacity();
                    if cap == 0 {
                        // Ensure capacity is greater than 0.
                        let mut raw = raw;
                        raw.set_capacity(1);
                        cache::$name::new(raw)
                    } else {
                        cache::$name::new(raw)
                    }
                };
                Self { store, cache }
            }

            pub fn get<T>(&self, key: T) -> io::Result<Option<$ptr<bits::Set>>>
            where
                T: AsRef<[u8]>,
            {
                let key_ref = key.as_ref();
                if let Some(set_ptr) = self.cache.get(key_ref) {
                    return Ok(Some(set_ptr));
                }

                if let Some(set) = self.store.borrow().get(key_ref)? {
                    let ptr = $ptr::new(set);
                    let dropped = self.cache.put(key_ref.to_vec(), ptr.clone());
                    if let Some(dropped) = dropped {
                        self.store
                            .borrow()
                            .put(dropped.0, &dropped.1)?;
                    }
                    Ok(Some(ptr))
                } else {
                    Ok(None)
                }
            }

            pub fn put<T>(&mut self, key: T, set: bits::Set) -> io::Result<()>
            where
                T: AsRef<[u8]>,
            {
                self.cache_put(key, $ptr::new(set))
            }

            pub fn snapshot(&self) -> io::Result<()> {
                self.cache.for_each(|(key, ptr)| {
                    self.store_put(key, &*ptr)
                })
            }

            fn cache_put<T>(&self, key: T, ptr: $ptr<bits::Set>) -> io::Result<()>
            where
                T: AsRef<[u8]>,
            {
                let key_ref = key.as_ref();
                if let Some(out) = self.cache.put(key_ref.to_vec(), ptr) {
                    self.store
                        .borrow()
                        .put(out.0, &out.1)?;
                }
                Ok(())
            }

            fn store_put<T>(&self, key: T, set: &bits::Set) -> io::Result<()>
            where
                T: AsRef<[u8]>,
            {
                self.store
                    .borrow()
                    .put(key, set)
            }

            pub fn seek(&self) -> Seek {
                self.store.borrow().seek()
            }
        }
    }
}

impls!(Index, Single, Rc);
impls!(SharedIndex, Shared, Arc);
