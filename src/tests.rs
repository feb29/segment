use std::fs;
use std::rc::Rc;
use std::sync::Arc;
use super::*;

#[test]
fn bucket_ops() {
    let path = "./test_bucket_ops";

    {
        let store = {
            let store = Store::open(path).unwrap();
            store.put("1", &bitset![1]).unwrap();
            store.put("2", &bitset![2]).unwrap();
            store.put("3", &bitset![3]).unwrap();
            store
        };
        let cache = cache::Raw::new(3);
        let index = Index::new(&store, cache);

        let got1 = index.get("1").unwrap().unwrap();
        let got2 = index.get("2").unwrap().unwrap();
        let got3 = index.get("3").unwrap().unwrap();
        assert_eq!(*got1, bitset![1]);
        assert_eq!(*got2, bitset![2]);
        assert_eq!(*got3, bitset![3]);
    }

    {
        let store = Rc::new(Store::open(path).unwrap());
        let cache = cache::Raw::new(1);
        let mut index = Index::new(store.clone(), cache);

        let got_a = index.get("1").unwrap().unwrap();
        let mut got_x = index.get("1").unwrap().unwrap();

        assert!(Rc::strong_count(&got_a) == 3); // cache, got_a and got_x
        assert!(!Rc::make_mut(&mut got_x).insert(42));
        assert!(Rc::strong_count(&got_a) == 2); // Rc::make_mut
        assert!(Rc::make_mut(&mut got_x).remove(42));
        assert!(Rc::strong_count(&got_a) == 2);

        let got_b = index.get("1").unwrap().unwrap();
        assert!(Rc::strong_count(&got_a) == 3); // cache, got_a and got_b
        assert!(Rc::strong_count(&got_b) == 3); // cache, got_a and got_b

        let got_c = index.get("2").unwrap().unwrap(); // expire key('1')
        assert!(Rc::strong_count(&got_a) == 2); // got_a and got_b
        assert!(Rc::strong_count(&got_b) == 2); // got_a and got_b
        assert!(Rc::strong_count(&got_c) == 2); // cache and got_c

        let mut got_y = index.get("3").unwrap().unwrap();
        let got_d = index.get("3").unwrap().unwrap(); // expire key('2')
        assert!(!Rc::make_mut(&mut got_y).insert(10));
        assert!(Rc::strong_count(&got_c) == 1); // got_c
        assert!(Rc::strong_count(&got_d) == 2); // cache and got_d, view_2 is vacant

        index.put("3", bitset![10000]).unwrap();
        assert!(!got_d.get(10000));
        assert!(Rc::strong_count(&got_d) == 1); // got_d, cache expired old ptr
    }

    {
        let store = Arc::new(Store::open(path).unwrap());
        let cache = cache::Raw::new(10);
        let index = Index::new(store.clone(), cache);

        let got1 = index.get("1").unwrap().unwrap();
        let got2 = index.get("2").unwrap().unwrap();
        let got3 = index.get("3").unwrap().unwrap();
        assert_eq!(*got1, bitset![1]);
        assert_eq!(*got2, bitset![2]);
        assert_eq!(*got3, bitset![3]);
    }

    {
        let store = Box::new(Store::open(path).unwrap());
        let cache = cache::Raw::new(0);
        let index = Index::new(store, cache);

        let got1 = index.get("1").unwrap().unwrap();
        let got2 = index.get("2").unwrap().unwrap();
        let got3 = index.get("3").unwrap().unwrap();
        assert_eq!(*got1, bitset![1]);
        assert_eq!(*got2, bitset![2]);
        assert_eq!(*got3, bitset![3]);
    }

    {
        let store = Store::open(path).unwrap();
        let cache = cache::Raw::new(0);
        let index = Index::new(&store, cache);

        let got1 = index.get(&b"1".to_vec()).unwrap().unwrap();
        let got2 = index.get(&b"2".to_vec()).unwrap().unwrap();
        let got3 = index.get(&b"3".to_vec()).unwrap().unwrap();
        assert_eq!(
            vec![1, 2, 3],
            got1.or(&*got2).or(&*got3).bits().collect::<Vec<_>>()
        );
    }

    assert!(fs::remove_dir_all(path).is_ok());
}

#[test]
fn cache_ops() {
    {
        let mut cache = cache::Raw::new(1);
        assert_eq!(cache.put("1", 1), None);
        assert!(cache.get("1").is_some());
        assert!(cache.get("2").is_none());

        assert_eq!(cache.put("2", 2), Some(("1", 1)),);
        assert!(cache.get("1").is_none());
        assert!(cache.get("2").is_some());
    }
    {
        let mut cache = cache::Raw::new(2);
        cache.put(1, 10);
        cache.put(2, 20);
        assert_eq!(cache.get(&1), Some(&mut 10));
        assert_eq!(cache[&1], 10);
        assert_eq!(cache.lru(), Some((&2, &20)));
        assert_eq!(cache.get(&2), Some(&mut 20));
        assert_eq!(cache[&2], 20);
        assert_eq!(cache.lru(), Some((&1, &10)));
        assert_eq!(cache.len(), 2);
    }
    {
        let mut cache = cache::Raw::new(1);
        cache.put("1", 10);
        cache.put("1", 19);
        assert_eq!(cache.get("1"), Some(&mut 19));
        assert_eq!(cache.len(), 1);
        cache["1"] = 20;
        assert_eq!(cache.get("1"), Some(&mut 20));
    }
    {
        let mut cache = cache::Raw::new(2);
        cache.put("1", 1);
        cache.put("2", 2);
        cache.put("3", 3);
        assert!(cache.get("1").is_none());

        cache.put("2", 2);
        cache.put("4", 4);
        assert!(cache.get("3").is_none());
        assert_eq!(cache.lru(), Some((&"2", &2)));
    }
    {
        let mut cache = cache::Raw::new(3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.put(4, 40);
        cache.put(5, 50);
        assert_eq!(
            cache.iter().collect::<Vec<_>>(),
            vec![(&3, &30), (&4, &40), (&5, &50)]
        );
    }
}

#[test]
fn store_ops() {
    let path = "./test_store_ops";

    {
        let store = {
            let store = Store::open(path).unwrap();
            store.put("10", &bitset![1]).unwrap();
            store.put("20", &bitset![2]).unwrap();
            store.put("30", &bitset![3]).unwrap();
            store
        };

        let got1 = store.get("10").unwrap().unwrap();
        let got2 = store.get("20").unwrap().unwrap();
        let got3 = store.get("30").unwrap().unwrap();
        assert_eq!(got1, bitset![1]);
        assert_eq!(got2, bitset![2]);
        assert_eq!(got3, bitset![3]);
    }

    {
        let store = Store::open(path).unwrap();
        let got1 = store.get("10").unwrap().unwrap();
        let got2 = store.get("20").unwrap().unwrap();
        let got3 = store.get("30").unwrap().unwrap();
        assert_eq!(got1, bitset![1]);
        assert_eq!(got2, bitset![2]);
        assert_eq!(got3, bitset![3]);
    }

    {
        let store = Store::open(path).unwrap();
        assert_eq!(bitset![1], store.seek().next("00").unwrap().unwrap().1);
        assert_eq!(bitset![1], store.seek().next("01").unwrap().unwrap().1);
        assert_eq!(bitset![1], store.seek().next("10").unwrap().unwrap().1);
        assert_eq!(bitset![2], store.seek().next("11").unwrap().unwrap().1);
        assert_eq!(bitset![2], store.seek().next("20").unwrap().unwrap().1);
        assert_eq!(bitset![3], store.seek().next("21").unwrap().unwrap().1);
        assert_eq!(bitset![3], store.seek().next("30").unwrap().unwrap().1);
        assert_eq!(None, store.seek().next("31").unwrap());
    }

    {
        let store = Store::open(path).unwrap();
        assert_eq!(None, store.seek().prev("00").unwrap());
        assert_eq!(None, store.seek().prev("01").unwrap());
        assert_eq!(bitset![1], store.seek().prev("10").unwrap().unwrap().1);
        assert_eq!(bitset![1], store.seek().prev("11").unwrap().unwrap().1);
        assert_eq!(bitset![2], store.seek().prev("20").unwrap().unwrap().1);
        assert_eq!(bitset![2], store.seek().prev("21").unwrap().unwrap().1);
        assert_eq!(bitset![3], store.seek().prev("30").unwrap().unwrap().1);
        assert_eq!(bitset![3], store.seek().prev("31").unwrap().unwrap().1);
    }

    assert!(fs::remove_dir_all(path).is_ok());
}
