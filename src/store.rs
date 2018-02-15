use std::io;
use std::path::Path;
use compacts::bits;
use rocksdb::{self, Writable};

#[derive(Debug)]
pub struct Store {
    db: rocksdb::DB,
}
pub struct Seek<'a> {
    db: &'a rocksdb::DB,
}

fn error_invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}
fn error_other(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

impl Store {
    // const NS_INDEX: &'static str = "INDEX";

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let db = {
            let path = path.as_ref().to_str().unwrap();
            let mut opts = rocksdb::DBOptions::new();
            opts.create_if_missing(true);
            rocksdb::DB::open(opts, path).map_err(error_invalid_input)?
        };
        // db.create_cf(Self::NS_INDEX).map_err(error_invalid_input)?;
        Ok(Store { db })
    }

    // pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    //     let db = {
    //         let path = path.as_ref().to_str().unwrap();
    //         let opts = rocksdb::DBOptions::new();
    //         let cfds = vec![Self::NS_INDEX];
    //         rocksdb::DB::open_cf(opts, path, cfds).map_err(error_invalid_input)?
    //     };
    //     Ok(Store { db })
    // }

    pub fn get<T>(&self, key: T) -> io::Result<Option<bits::Set>>
    where
        T: AsRef<[u8]>,
    {
        let opt = self.db.get(key.as_ref()).map_err(error_other)?;
        if let Some(db_vec) = opt {
            // from `rocksdb::DBVector` to `Vec<u8>`
            let mut vec = db_vec.to_vec();
            let set = bits::Set::read_from(&mut io::Cursor::new(vec))?;
            Ok(Some(set))
        } else {
            Ok(None)
        }
    }

    pub fn put<T>(&self, key: T, set: &bits::Set) -> io::Result<()>
    where
        T: AsRef<[u8]>,
    {
        let vec = {
            let mut buf = Vec::with_capacity(1024);
            set.write_to(&mut buf)?;
            buf
        };
        self.db.put(key.as_ref(), &vec[..]).map_err(error_other)?;
        Ok(())
    }

    pub fn seek(&self) -> Seek {
        Seek { db: &self.db }
    }
}

impl<'a> Seek<'a> {
    pub fn next<T>(&self, t: T) -> io::Result<Option<(Vec<u8>, bits::Set)>>
    where
        T: AsRef<[u8]>,
    {
        let mut iter = self.db.iter();
        let seek_key = rocksdb::SeekKey::Key(t.as_ref());
        if iter.seek(seek_key) {
            let key = iter.key();
            let val = iter.value();
            let set = bits::Set::read_from(&mut io::Cursor::new(val))?;
            Ok(Some((key.to_vec(), set)))
        } else {
            Ok(None)
        }
    }

    pub fn prev<T>(&self, t: T) -> io::Result<Option<(Vec<u8>, bits::Set)>>
    where
        T: AsRef<[u8]>,
    {
        let mut iter = self.db.iter();
        let seek_key = rocksdb::SeekKey::Key(t.as_ref());
        if iter.seek_for_prev(seek_key) {
            let key = iter.key();
            let val = iter.value();
            let set = bits::Set::read_from(&mut io::Cursor::new(val))?;
            Ok(Some((key.to_vec(), set)))
        } else {
            Ok(None)
        }
    }
}
