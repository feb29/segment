extern crate segment;

#[test]
fn test() {
    let tab: segment::Table<u32> = segment::Table::new();
    println!("{:?}", tab);
}
