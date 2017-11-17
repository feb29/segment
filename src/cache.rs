use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
pub use std::collections::hash_map::RandomState;
use std::ops;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;
use linked_hash_map::{self, LinkedHashMap};
use parking_lot::Mutex;

pub trait Cache<K, V> {
    /// Returns the value corresponding to the given key.
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq;

    /// Insert a key-value pair, and return the dropped entry.
    fn put(&self, k: K, v: V) -> Option<(K, V)>;
}

#[derive(Debug)]
pub struct Single<K: Eq + Hash, V, S: BuildHasher>(RefCell<Raw<K, Rc<V>, S>>);

#[derive(Debug)]
pub struct Shared<K: Eq + Hash, V, S: BuildHasher>(Arc<Mutex<Raw<K, Arc<V>, S>>>);

impl<K, V, S> Single<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn new(raw: Raw<K, Rc<V>, S>) -> Self {
        Single(RefCell::new(raw))
    }
}
impl<K, V, S> Cache<K, Rc<V>> for Single<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<Rc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut raw = self.0.borrow_mut();
        raw.get(k).map(|ptr| Rc::clone(ptr))
    }

    fn put(&self, k: K, v: Rc<V>) -> Option<(K, Rc<V>)> {
        let mut raw = self.0.borrow_mut();
        raw.put(k, v)
    }
}
impl<K, V, S> Single<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) fn for_each<F, E>(&self, f: F) -> Result<(), E>
    where
        F: Fn((&K, &Rc<V>)) -> Result<(), E>,
    {
        let raw = self.0.borrow();
        for elem in raw.iter() {
            f(elem)?;
        }
        Ok(())
    }
}

impl<K, V, S> Shared<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn new(raw: Raw<K, Arc<V>, S>) -> Self {
        Shared(Arc::new(Mutex::new(raw)))
    }
}
impl<K, V, S> Cache<K, Arc<V>> for Shared<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get<Q: ?Sized>(&self, k: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut raw = self.0.lock();
        raw.get(k).map(|ptr| Arc::clone(ptr))
    }

    fn put(&self, k: K, v: Arc<V>) -> Option<(K, Arc<V>)> {
        let mut raw = self.0.lock();
        raw.put(k, v)
    }
}
impl<K, V, S> Shared<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) fn for_each<F, E>(&self, f: F) -> Result<(), E>
    where
        F: Fn((&K, &Arc<V>)) -> Result<(), E>,
    {
        let raw = self.0.lock();
        for elem in raw.iter() {
            f(elem)?;
        }
        Ok(())
    }
}

/// A raw LRU cache.
#[derive(Debug, Clone)]
pub struct Raw<K, V, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    map: LinkedHashMap<K, V, S>,
    cap: usize,
}

impl<K, V> Raw<K, V>
where
    K: Eq + Hash,
{
    /// Creates an empty cache that can hold at most `capacity` items.
    pub fn new(cap: usize) -> Self {
        let map = LinkedHashMap::new();
        Self { map, cap }
    }
}

impl<K, V, S> Raw<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Creates an empty cache that can hold at most `capacity` items
    /// with the given hash builder.
    pub fn with_hasher(cap: usize, hash_builder: S) -> Self {
        let map = LinkedHashMap::with_hasher(hash_builder);
        Self { map, cap }
    }

    /// Returns the number of key-value pairs in the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the cache contains no key-value pairs.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the maximum number of key-value pairs the cache can hold.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Removes all key-value pairs from the cache.
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Checks if cache contains the given key.
    /// This does _not_ affect the cache's LRU state.
    pub fn exists<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.contains_key(k)
    }

    /// Insert a key-value pair into cache. If the key exists, the old value is returned.
    /// This does _not_ affect the cache's LRU state.
    /// Use `put` to ensure that `capacity` is greater than `length`.
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.map.insert(k, v)
    }

    /// Remove a key-value pair from cache.
    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.remove(k)
    }

    /// Sets the number of key-value pairs the cache can hold. Removes
    /// least-recently-used key-value pairs if necessary.
    pub fn set_capacity(&mut self, capacity: usize) {
        for _ in capacity..self.len() {
            self.pop_lru();
        }
        self.cap = capacity;
    }

    /// Returns a mutable reference to the value corresponding to the given key,
    /// If value is found, it is moved to the end of the cache.
    pub fn get<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get_refresh(k)
    }

    // /// Returns a mutable reference to the value corresponding to the given key,
    // pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    // where
    //     K: Borrow<Q>,
    //     Q: Hash + Eq,
    // {
    //     self.map.get_mut(k)
    // }

    /// Insert a key-value pair, and return the dropped entry.
    pub fn put(&mut self, k: K, v: V) -> Option<(K, V)> {
        let _swapped = self.map.insert(k, v);
        if self.len() > self.capacity() {
            self.pop_lru()
        } else {
            None
        }
    }

    /// Return the least recently used entry.
    #[inline]
    pub fn lru(&self) -> Option<(&K, &V)> {
        self.map.front()
    }

    /// Return the most recently used entry.
    #[inline]
    pub fn mru(&self) -> Option<(&K, &V)> {
        self.map.back()
    }

    /// Removes and returns the least recently used entry.
    #[inline]
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        self.map.pop_front()
    }

    /// Removes and returns the most recently used entry.
    #[inline]
    pub fn pop_mru(&mut self) -> Option<(K, V)> {
        self.map.pop_back()
    }

    /// Returns an iterator key-value pairs
    /// in least-recently-used to most-recently-used order.
    /// Accessing the cache through the iterator does _not_ affect the cache's LRU state.
    pub fn iter(&self) -> Iter<K, V> {
        Iter(self.map.iter())
    }

    /// Returns a mutable iterator over the cache's key-value pairs in
    /// least-recently-used to most-recently-used order,
    /// Accessing the cache through the iterator does _not_ affect the cache's LRU state.
    pub fn iter_mut(&mut self) -> IterMut<K, V> {
        IterMut(self.map.iter_mut())
    }
}

impl<'key, K, V, S, Q> ops::Index<&'key Q> for Raw<K, V, S>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    type Output = V;
    /// Returns a reference to the value corresponding to the given key.
    /// This does _not_ affect the cache's LRU state.
    fn index(&self, i: &Q) -> &Self::Output {
        self.map.get(i).expect("cache entry not found")
    }
}

impl<'key, K, V, S, Q> ops::IndexMut<&'key Q> for Raw<K, V, S>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    /// Returns a mutable reference to the value corresponding to the given key.
    /// This does _not_ affect the cache's LRU state.
    fn index_mut(&mut self, i: &Q) -> &mut V {
        self.map.get_mut(i).expect("cache entry not found")
    }
}

/// An iterator over a cache's key-value pairs
/// in least-recently-used to most-recently-used order.
#[derive(Clone)]
pub struct Iter<'a, K: 'a, V: 'a>(linked_hash_map::Iter<'a, K, V>);

/// A mutable iterator over a cache's key-value pairs
/// in least-recently-used to most-recently-used order.
pub struct IterMut<'a, K: 'a, V: 'a>(linked_hash_map::IterMut<'a, K, V>);

/// An iterator over a cache's key-value pairs
/// in least-recently-used to most-recently-used order.
#[derive(Clone)]
pub struct IntoIter<K, V>(linked_hash_map::IntoIter<K, V>);

impl<'a, K, V, S> IntoIterator for &'a Raw<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<'a, K, V, S> IntoIterator for &'a mut Raw<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;
    fn into_iter(self) -> IterMut<'a, K, V> {
        self.iter_mut()
    }
}

impl<K, V, S> IntoIterator for Raw<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter(self.map.into_iter())
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> {
        self.0.next_back()
    }
}
impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);
    fn next(&mut self) -> Option<(&'a K, &'a mut V)> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<'a, K, V> DoubleEndedIterator for IterMut<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a mut V)> {
        self.0.next_back()
    }
}
impl<'a, K, V> ExactSizeIterator for IterMut<'a, K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);
    fn next(&mut self) -> Option<(K, V)> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<K, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        self.0.next_back()
    }
}
impl<K, V> ExactSizeIterator for IntoIter<K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}
