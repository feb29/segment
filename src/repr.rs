use compacts::bits;

/// Repr<T>
#[derive(Debug, Clone)]
pub struct Repr<T> {
    inner: Inner<T>,
}
#[derive(Debug, Clone)]
enum Inner<T> {
    Mem(bits::Set),
    Key(T),
}
