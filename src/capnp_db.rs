#![allow(dead_code)]
use capnp;
use crate::error::{Error, Result};
use lmdb::{self, Cursor, Transaction, EnvironmentFlags};
use std::path::Path;
use std::result::Result as StdResult;

pub type MessageReader<'a, T> = capnp::message::TypedReader<capnp::serialize::SliceSegments<'a>, T>;

pub struct Environment {
    env: lmdb::Environment,
}
pub struct RwTransaction<'a> {
    txn: lmdb::RwTransaction<'a>,
    pub dirty: bool,
}
pub type RoTransaction<'a> = lmdb::RoTransaction<'a>;

pub struct Iter<'txn> {
    iter: lmdb::Iter<'txn>,
}

pub trait CapnpCursor<'txn> {
    fn capnp_iter_start(&mut self) -> Iter<'txn>;
    fn capnp_iter_from<K>(&mut self, key: &K) -> Iter<'txn>
    where
        K: AsRef<[u8]>;
}
impl<'txn> CapnpCursor<'txn> for lmdb::RoCursor<'txn> {
    fn capnp_iter_start(&mut self) -> Iter<'txn> {
        Iter {
            iter: self.iter_start(),
        }
    }
    fn capnp_iter_from<K>(&mut self, key: &K) -> Iter<'txn>
    where
        K: AsRef<[u8]>,
    {
        Iter {
            iter: self.iter_from(key),
        }
    }
}

impl<'txn> Iterator for Iter<'txn> {
    type Item = (
        &'txn [u8],
        StdResult<capnp::message::Reader<capnp::serialize::SliceSegments<'txn>>, capnp::Error>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        let (key_bytes, value_bytes) = self.iter.next()?;
        let slice;
        unsafe {
            slice = capnp::Word::bytes_to_words(value_bytes);
        }
        let value_msg = capnp::serialize::read_message_from_words(
            &slice,
            capnp::message::ReaderOptions::default(),
        );
        Some((key_bytes, value_msg))
    }
}

pub trait DBTransaction<'a, T: lmdb::Transaction + 'a>: Sized {
    fn txn(&'a self) -> &'a T;

    fn open_ro_cursor(&'a self, db: lmdb::Database) -> Result<lmdb::RoCursor<'a>> {
        Ok(self.txn().open_ro_cursor(db)?)
    }

    fn get<V: for<'b> capnp::traits::Owned<'b>, K>(
        &'a self,
        db: lmdb::Database,
        key: &K,
    ) -> Result<Option<MessageReader<'a, V>>>
    where
        K: AsRef<[u8]>,
    {
        let get_result = self.txn().get(db, key);
        if get_result.is_err() {
            Ok(None)
        } else {
            let slice;
            unsafe {
                slice = capnp::Word::bytes_to_words(get_result.unwrap());
            }
            let msg = capnp::serialize::read_message_from_words(
                slice,
                capnp::message::ReaderOptions::default(),
            )?
            .into_typed::<V>();
            Ok(Some(msg))
        }
    }
    fn get_as_bytes<K>(&'a self, db: lmdb::Database, key: &K) -> Result<Option<&[u8]>>
    where
        K: AsRef<[u8]>,
    {
        let get_result = self.txn().get(db, key);
        if get_result.is_err() {
            Ok(None)
        } else {
            Ok(Some(get_result.unwrap()))
        }
    }
    fn get_capnp<K: capnp::message::Allocator, V: for<'b> capnp::traits::Owned<'b>>(
        &'a self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
    ) -> Result<Option<MessageReader<'a, V>>> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        let get_result = self.txn().get(db, &capnp::Word::words_to_bytes(&key_vec));
        if get_result.is_err() {
            Ok(None)
        } else {
            let slice;
            unsafe {
                slice = capnp::Word::bytes_to_words(get_result.unwrap());
            }
            let msg = capnp::serialize::read_message_from_words(
                slice,
                capnp::message::ReaderOptions::default(),
            )?
            .into_typed::<V>();
            Ok(Some(msg))
        }
    }
}

impl<'a> DBTransaction<'a, lmdb::RwTransaction<'a>> for RwTransaction<'a> {
    fn txn(&'a self) -> &'a lmdb::RwTransaction<'a> {
        &self.txn
    }
}

impl<'a> DBTransaction<'a, lmdb::RoTransaction<'a>> for RoTransaction<'a> {
    fn txn(&'a self) -> &'a lmdb::RoTransaction<'a> {
        &self
    }
}

impl<'a> RwTransaction<'a> {
    pub fn put<K, V: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &K,
        value: &capnp::message::Builder<V>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let value_vec = capnp::serialize::write_message_to_words(value);
        self.txn.put(
            db,
            key,
            &capnp::Word::words_to_bytes(&value_vec),
            lmdb::WriteFlags::default(),
        )?;
        self.dirty = true;
        Ok(())
    }

    pub fn put_capnp<K: capnp::message::Allocator, V: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
        value: &capnp::message::Builder<V>,
    ) -> Result<()> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        let value_vec = capnp::serialize::write_message_to_words(value);
        self.txn.put(
            db,
            &capnp::Word::words_to_bytes(&key_vec),
            &capnp::Word::words_to_bytes(&value_vec),
            lmdb::WriteFlags::default(),
        )?;
        self.dirty = true;
        Ok(())
    }
    pub fn put_bytes<K, V>(&mut self, db: lmdb::Database, key: &K, value: &V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.txn.put(db, key, value, lmdb::WriteFlags::default())?;
        self.dirty = true;
        Ok(())
    }
    pub fn delete<K>(&mut self, db: lmdb::Database, key: &K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        self.dirty = true;
        match self.txn.del(db, key, Option::None) {
            Err(err) => match err {
                lmdb::Error::NotFound => Ok(false),
                _ => Err(Error::LMDB(err)),
            },
            Ok(_) => Ok(true),
        }
    }

    pub fn delete_capnp<K: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
    ) -> Result<()> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        self.txn
            .del(db, &capnp::Word::words_to_bytes(&key_vec), Option::None)?;
        self.dirty = true;
        Ok(())
    }

    pub fn clear_db(&mut self, db: lmdb::Database) -> Result<()> {
        self.txn.clear_db(db)?;
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()?;
        Ok(())
    }

    pub fn open_rw_cursor(&'a mut self, db: lmdb::Database) -> Result<lmdb::RwCursor<'a>> {
        Ok(self.txn.open_rw_cursor(db)?)
    }
}

impl Environment {
    pub fn new(path: &Path) -> Result<Environment> {
        #[cfg(target_pointer_width = "32")]
        let map_size = 1 << 31;
        #[cfg(target_pointer_width = "64")]
        let map_size = 1 << 42;
        let env = lmdb::Environment::new()
            .set_max_dbs(64)
            .set_map_size(map_size)
            .open(path)?;
        Ok(Environment { env })
    }

    pub fn create_db(
        &self,
        name: Option<&str>,
        flags: lmdb::DatabaseFlags,
    ) -> Result<lmdb::Database> {
        Ok(self.env.create_db(name, flags)?)
    }

    pub fn rw_txn(&self) -> Result<RwTransaction> {
        let txn = self.env.begin_rw_txn()?;
        Ok(RwTransaction { txn, dirty: false })
    }
    pub fn ro_txn(&self) -> Result<RoTransaction> {
        Ok(self.env.begin_ro_txn()?)
    }
}
